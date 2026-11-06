import os

os.environ.setdefault("JWT_SECRET", "test-secret-test-secret-test-secret-123")

import fakeredis.aioredis
import pytest
from engagehub_common.messaging.memory import InMemoryKafka
from engagehub_common.session.log import ConversationLog
from engagehub_common.session.store import SessionState, SessionStore


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
def conversation_log(store, kafka):
    return ConversationLog(store, kafka.producer(), "test")


@pytest.fixture
async def session(store):
    state = SessionState(conversation_id="c1", session_id="s1", tenant_id="t1", customer_id="cust1")
    await store.create(state)
    return state
