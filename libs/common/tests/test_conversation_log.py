"""Proves the reserve -> publish -> commit protocol under failures."""

import pytest
from engagehub_common.events import EventType
from engagehub_common.messaging.bus import PublishError
from engagehub_common.session.log import SessionNotFound, StaleConnection
from engagehub_common.topics import CHAT_MESSAGES


def append(log, key="k1", text="hello", **kwargs):
    return log.append(
        tenant_id="t1",
        conversation_id="c1",
        customer_id="cust1",
        event_type=EventType.CUSTOMER_MESSAGE,
        payload={"text": text},
        idempotency_key=key,
        **kwargs,
    )


async def test_sequence_numbers_increase_and_messages_are_buffered(conversation_log, store, session):
    first = await append(conversation_log, "k1")
    second = await append(conversation_log, "k2")
    assert (first.event.sequence_number, second.event.sequence_number) == (1, 2)
    assert first.event.message_id != second.event.message_id
    assert len(await store.hot_log_after("t1", "c1", 0)) == 2
    state = await store.get("t1", "c1")
    assert state.seq == 2 and state.last_committed_seq == 2


async def test_retry_with_same_key_keeps_identity_and_does_not_republish(conversation_log, kafka, session):
    first = await append(conversation_log, "k1")
    retry = await append(conversation_log, "k1")
    assert retry.duplicate is True
    assert retry.event.message_id == first.event.message_id
    assert retry.event.sequence_number == first.event.sequence_number
    assert len(kafka.events(CHAT_MESSAGES)) == 1


async def test_failed_publish_is_retried_with_the_same_identity(conversation_log, kafka, store, session):
    kafka.fail_publishes = 1
    with pytest.raises(PublishError):
        await append(conversation_log, "k1")
    assert len(kafka.events(CHAT_MESSAGES)) == 0
    assert list(await store.pending("t1", "c1")) == [1]

    result = await append(conversation_log, "k1")
    assert result.event.sequence_number == 1
    assert len(kafka.events(CHAT_MESSAGES)) == 1
    assert await store.pending("t1", "c1") == {}


async def test_crash_after_publish_before_commit_produces_identical_duplicate(
    conversation_log, kafka, store, session, monkeypatch
):
    """The writer dies after Kafka confirmed. The retry's record has the same IDs."""
    real_commit = store.commit

    async def crash(*args, **kwargs):
        raise ConnectionError("pod died")

    monkeypatch.setattr(store, "commit", crash)
    with pytest.raises(ConnectionError):
        await append(conversation_log, "k1")
    monkeypatch.setattr(store, "commit", real_commit)

    await append(conversation_log, "k1")
    events = kafka.events(CHAT_MESSAGES)
    assert len(events) == 2
    assert events[0].event_id == events[1].event_id
    assert events[0].message_id == events[1].message_id
    assert events[0].sequence_number == events[1].sequence_number
    assert len(await store.hot_log_after("t1", "c1", 0)) == 1


async def test_reconcile_republishes_abandoned_reservations_only(conversation_log, kafka, store, session):
    kafka.fail_publishes = 1
    with pytest.raises(PublishError):
        await append(conversation_log, "k1")
    pending = await store.pending("t1", "c1")
    reserved_at = pending[1][0]

    count, in_flight = await conversation_log.reconcile_pending("t1", "c1", older_than_ms=1000, now_ms=reserved_at + 10)
    assert (count, in_flight) == (0, 1)
    assert kafka.events(CHAT_MESSAGES) == []

    count, in_flight = await conversation_log.reconcile_pending(
        "t1", "c1", older_than_ms=1000, now_ms=reserved_at + 5000
    )
    assert (count, in_flight) == (1, None)
    assert [e.sequence_number for e in kafka.events(CHAT_MESSAGES)] == [1]
    assert await store.pending("t1", "c1") == {}


async def test_missing_session_raises_without_restore(conversation_log):
    with pytest.raises(SessionNotFound):
        await append(conversation_log, "k1")


async def test_restore_callback_recovers_a_missing_session(store, kafka, session):
    from engagehub_common.session.log import ConversationLog
    from engagehub_common.session.store import SessionState

    await store.delete_all("t1", "c1")
    calls = []

    async def restore(tenant, conv):
        calls.append((tenant, conv))
        await store.restore(
            SessionState(conversation_id=conv, session_id="s", tenant_id=tenant, customer_id="cust1", seq=7)
        )
        return True

    log = ConversationLog(store, kafka.producer(), "test", restore=restore)
    result = await append(log, "k9")
    assert calls == [("t1", "c1")]
    assert result.event.sequence_number == 8


async def test_superseded_connection_is_fenced(conversation_log, store, session):
    old_epoch, _ = await store.connect("t1", "c1", "cust1", "pod-a", True)
    new_epoch, previous = await store.connect("t1", "c1", "cust1", "pod-b", True)
    assert new_epoch == old_epoch + 1 and previous == "pod-a"
    with pytest.raises(StaleConnection):
        await append(conversation_log, "k1", participant="cust1", epoch=old_epoch)
    result = await append(conversation_log, "k1", participant="cust1", epoch=new_epoch)
    assert result.event.sequence_number == 1


async def test_clean_disconnect_clears_the_previous_pod(store, session):
    epoch, _ = await store.connect("t1", "c1", "cust1", "pod-a", True)
    await store.disconnect("t1", "c1", "cust1", epoch)
    _, previous = await store.connect("t1", "c1", "cust1", "pod-b", True)
    assert previous == ""


async def test_customer_ack_trims_buffer_and_rejects_values_beyond_head(conversation_log, store, session):
    for i in range(3):
        await append(conversation_log, f"k{i}")
    assert await store.record_ack("t1", "c1", "cust1", 99, is_customer=True) == -1
    assert await store.record_ack("t1", "c1", "cust1", 2, is_customer=True, message_id="m2") == 2
    assert await store.record_ack("t1", "c1", "cust1", 1, is_customer=True) == 2  # never moves backward
    state = await store.get("t1", "c1")
    assert state.last_ack_seq == 2 and state.last_ack_message_id == "m2"
    assert await store.unacked_count("t1", "c1") == 1


async def test_agent_ack_does_not_trim_customer_buffer(conversation_log, store, session):
    await append(conversation_log, "k1")
    await store.record_ack("t1", "c1", "agent1", 1, is_customer=False)
    assert await store.unacked_count("t1", "c1") == 1
    assert await store.participant_ack("t1", "c1", "agent1") == 1


async def test_tenant_scoped_keys_isolate_sessions(store, session):
    assert await store.get("other-tenant", "c1") is None
