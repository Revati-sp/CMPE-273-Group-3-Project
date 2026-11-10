"""Proves retry, dead-lettering, deduplication and safe restart behaviour."""

import pytest
from engagehub_common.events import EventEnvelope, EventType
from engagehub_common.messaging.consumer import ConsumerRunner, NonRetryableError, RedisDedupe
from engagehub_common.topics import DEAD_LETTER

TOPIC = "chat.notifications"


def event(conv="c1", event_id=None, **payload):
    fields = dict(
        event_type=EventType.NOTIFICATION_REQUESTED,
        conversation_id=conv,
        tenant_id="t1",
        producer_service="test",
        payload=payload,
    )
    if event_id:
        fields["event_id"] = event_id
    return EventEnvelope(**fields)


async def publish(kafka, *events):
    producer = kafka.producer()
    for e in events:
        await producer.publish(TOPIC, e)


async def make_runner(kafka, handler, redis=None, group="g1", **kwargs):
    consumer = kafka.consumer([TOPIC], group)
    await consumer.start()
    return ConsumerRunner(
        consumer,
        handler,
        group=group,
        producer=kafka.producer(),
        dedupe=RedisDedupe(redis, group) if redis else None,
        retry_base_seconds=0,
        **kwargs,
    )


async def test_events_in_one_conversation_are_handled_in_order_and_committed(kafka):
    handled = []

    async def handler(e, _):
        handled.append(e.payload["n"])

    await publish(kafka, *(event(n=i) for i in range(10)))
    runner = await make_runner(kafka, handler)
    await runner.process_once()
    assert handled == list(range(10))
    partition = kafka.partition_for(b"c1")
    assert kafka.committed[("g1", (TOPIC, partition))] == 10


async def test_transient_failure_is_retried_before_later_events(kafka):
    attempts = {"a": 0}
    handled = []

    async def handler(e, _):
        if e.payload["n"] == "a" and attempts["a"] < 2:
            attempts["a"] += 1
            raise ConnectionError("transient")
        handled.append(e.payload["n"])

    await publish(kafka, event(n="a"), event(n="b"))
    runner = await make_runner(kafka, handler)
    await runner.process_once()
    assert handled == ["a", "b"]
    assert kafka.events(DEAD_LETTER) == []


async def test_poison_event_goes_to_dlq_and_later_events_still_flow(kafka):
    handled = []

    async def handler(e, _):
        if e.payload["n"] == "poison":
            raise ValueError("cannot process")
        handled.append(e.payload["n"])

    await publish(kafka, event(n="poison"), event(n="good"))
    runner = await make_runner(kafka, handler)
    await runner.process_once()
    assert handled == ["good"]
    letters = kafka.events(DEAD_LETTER)
    assert len(letters) == 1
    assert letters[0].payload["source_topic"] == TOPIC
    assert letters[0].payload["original"]["payload"]["n"] == "poison"
    assert "handler_failed" in letters[0].payload["reason"]


async def test_non_retryable_error_skips_retries(kafka):
    calls = []

    async def handler(e, _):
        calls.append(1)
        raise NonRetryableError("bad data")

    await publish(kafka, event())
    runner = await make_runner(kafka, handler)
    await runner.process_once()
    assert len(calls) == 1
    assert len(kafka.events(DEAD_LETTER)) == 1


async def test_schema_invalid_record_is_dead_lettered_without_calling_handler(kafka):
    calls = []

    async def handler(e, _):
        calls.append(e)

    kafka.append(TOPIC, b"c1", b'{"not":"an envelope"}')
    kafka.append(TOPIC, b"c1", b"\xff\xfe garbage")
    runner = await make_runner(kafka, handler)
    await runner.process_once()
    assert calls == []
    letters = kafka.events(DEAD_LETTER)
    assert len(letters) == 2
    assert all("schema_validation" in letter.payload["reason"] for letter in letters)


async def test_dlq_publish_failure_leaves_the_offset_uncommitted(kafka):
    async def handler(e, _):
        raise ValueError("boom")

    await publish(kafka, event())
    runner = await make_runner(kafka, handler)
    kafka.fail_publishes = 1
    with pytest.raises(Exception, match="injected"):
        await runner.process_once()
    assert kafka.committed == {}


async def test_redelivered_event_id_is_suppressed_by_dedupe(kafka, redis):
    handled = []

    async def handler(e, _):
        handled.append(e.event_id)

    await publish(kafka, event(event_id="same"), event(event_id="same"), event(event_id="other"))
    runner = await make_runner(kafka, handler, redis)
    await runner.process_once()
    assert handled == ["same", "other"]


async def test_restart_before_commit_redelivers_but_does_not_reprocess(kafka, redis):
    """Crash after the handler succeeded but before the commit."""
    handled = []

    async def handler(e, _):
        handled.append(e.payload["n"])

    await publish(kafka, event(n=1), event(n=2))
    crashed = await make_runner(kafka, handler, redis)
    crashed.commit = False
    await crashed.process_once()
    assert handled == [1, 2]
    assert kafka.committed == {}

    restarted = await make_runner(kafka, handler, redis)
    assert await restarted.process_once() == 2  # redelivered by Kafka
    assert handled == [1, 2]  # but not processed twice


async def test_restart_resumes_from_committed_offset(kafka, redis):
    handled = []

    async def handler(e, _):
        handled.append(e.payload["n"])

    await publish(kafka, event(n=1))
    first = await make_runner(kafka, handler, redis)
    await first.process_once()
    await publish(kafka, event(n=2))
    second = await make_runner(kafka, handler, redis)
    assert await second.process_once() == 1
    assert handled == [1, 2]


async def test_independent_consumer_groups_each_receive_every_event(kafka):
    a, b = [], []

    async def handler_a(e, _):
        a.append(e.event_id)

    async def handler_b(e, _):
        b.append(e.event_id)

    await publish(kafka, event())
    await (await make_runner(kafka, handler_a, group="a")).process_once()
    await (await make_runner(kafka, handler_b, group="b")).process_once()
    assert a == b and len(a) == 1


async def test_redis_outage_does_not_stop_consumption(kafka):
    class BrokenRedis:
        async def exists(self, *_):
            raise ConnectionError

        async def set(self, *_, **__):
            raise ConnectionError

    handled = []

    async def handler(e, _):
        handled.append(1)

    await publish(kafka, event())
    runner = await make_runner(kafka, handler, BrokenRedis())
    await runner.process_once()
    assert handled == [1]


async def test_key_based_partitioning_keeps_a_conversation_on_one_partition(kafka):
    await publish(kafka, *(event(conv="c-ordered", n=i) for i in range(50)))
    populated = {p for (t, p), recs in kafka.logs.items() if t == TOPIC and recs}
    assert populated == {kafka.partition_for(b"c-ordered")}
