import os

os.environ.setdefault("JWT_SECRET", "test-secret-test-secret-test-secret-123")
os.environ.setdefault("OTEL_ENABLED", "false")

import fakeredis.aioredis
import pytest
from app.history import HistoryService
from app.models import Base
from engagehub_common.db import Database
from engagehub_common.messaging.memory import InMemoryKafka
from engagehub_common.session.store import SessionStore


@pytest.fixture
def kafka():
    return InMemoryKafka()


@pytest.fixture
async def db():
    database = Database("sqlite+aiosqlite:///:memory:")
    await database.create_schema(Base)
    yield database
    await database.dispose()


@pytest.fixture
async def redis():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
def store(redis):
    return SessionStore(redis)


@pytest.fixture
def history(db, store, kafka):
    return HistoryService(db, store, kafka.log_reader())
