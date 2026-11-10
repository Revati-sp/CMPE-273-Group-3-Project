import os

os.environ.setdefault("JWT_SECRET", "test-secret-test-secret-test-secret-123")
os.environ.setdefault("OTEL_ENABLED", "false")

import fakeredis.aioredis
import pytest
from engagehub_common.events import MESSAGE_EVENT_TYPES
from engagehub_common.messaging.memory import InMemoryKafka
from engagehub_common.session.history import HistoryUnavailable
from engagehub_common.session.store import SessionState, SessionStore
from engagehub_common.topics import CHAT_MESSAGES


class KafkaBackedHistory:
    """Stands in for session-service in these tests, reading the retained log."""

    def __init__(self, kafka, store):
        self.kafka, self.store = kafka, store
        self.available = True
        self.calls = 0

    async def messages_after(self, tenant_id, conversation_id, after_seq, limit=1000):
        self.calls += 1
        if not self.available:
            raise HistoryUnavailable("down")
        by_seq = {}
        for event in self.kafka.events(CHAT_MESSAGES):
            if (
                event.tenant_id == tenant_id
                and event.conversation_id == conversation_id
                and event.event_type in MESSAGE_EVENT_TYPES
                and event.sequence_number > after_seq
            ):
                by_seq.setdefault(event.sequence_number, event)
        return [by_seq[s] for s in sorted(by_seq)]

    async def restore_session(self, tenant_id, conversation_id):
        if not self.available:
            raise HistoryUnavailable("down")
        events = await self.messages_after(tenant_id, conversation_id, 0)
        if not events:
            return False
        head = events[-1].sequence_number
        await self.store.restore(
            SessionState(
                conversation_id=conversation_id,
                session_id="restored",
                tenant_id=tenant_id,
                customer_id=events[-1].customer_id,
                seq=head,
                last_committed_seq=head,
            )
        )
        return True


@pytest.fixture
def kafka():
    return InMemoryKafka()


@pytest.fixture
async def redis():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
def store(redis):
    return SessionStore(redis)


@pytest.fixture
def history(kafka, store):
    return KafkaBackedHistory(kafka, store)
