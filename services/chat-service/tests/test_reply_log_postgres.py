"""Concurrency on real PostgreSQL: the row lock must serialise sequence assignment.

SQLite serialises everything, so it cannot prove this. Run with:

    TEST_POSTGRES_URL=postgresql+asyncpg://user:pass@localhost:5432/eh_chat_test python -m pytest tests
"""

import asyncio
import os
from datetime import UTC, datetime

import pytest
from app.replies.log import MessageLog
from app.replies.models import ReplyBase
from engagehub_common.db import Database, ensure_database

URL = os.environ.get("TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(not URL, reason="set TEST_POSTGRES_URL to run against PostgreSQL")


@pytest.fixture
async def message_log():
    await ensure_database(URL)  # also exercises database creation on an existing server
    db = Database(URL)
    async with db.engine.begin() as conn:
        await conn.run_sync(ReplyBase.metadata.drop_all)
    await db.create_schema(ReplyBase)
    yield MessageLog(db)
    await db.dispose()


def append(log, conversation_id, n, message_id=None):
    return log.append(
        conversation_id=conversation_id, message_id=message_id or f"{conversation_id}-m{n}",
        sender_type="virtual_agent",
        sender_id="agent", sender_name="", content=str(n), timestamp=datetime.now(UTC), correlation_id="c",
    )  # fmt: skip


async def test_parallel_appends_on_separate_connections_get_contiguous_unique_sequences(message_log):
    await message_log.register("conv-a", "acme", "cust-1")
    await message_log.register("conv-b", "acme", "cust-1")
    tasks = [append(message_log, conv, n) for n in range(40) for conv in ("conv-a", "conv-b")]
    tasks += [append(message_log, "conv-a", n) for n in range(10)]  # redelivered duplicates, racing
    results = await asyncio.gather(*tasks)
    for conv in ("conv-a", "conv-b"):
        fresh = sorted(
            r.message.sequence_number for r in results if r.message.conversation_id == conv and not r.duplicate
        )
        assert fresh == list(range(1, 41)), conv
    assert sum(r.duplicate for r in results) == 10
    assert [m.sequence_number for m in await message_log.after("conv-a", 0)] == list(range(1, 41))


async def test_same_message_id_racing_into_two_conversations_is_rejected_once(message_log):
    from app.replies.log import MessageIdConflict

    await message_log.register("conv-a", "acme", "cust-1")
    await message_log.register("conv-b", "acme", "cust-1")
    outcomes = await asyncio.gather(
        append(message_log, "conv-a", 1, "shared-id"), append(message_log, "conv-b", 1, "shared-id"),
        return_exceptions=True,
    )  # fmt: skip
    assert sum(isinstance(o, MessageIdConflict) for o in outcomes) == 1
    assert await message_log.latest_sequence("conv-a") + await message_log.latest_sequence("conv-b") == 1


async def test_messages_carry_tenant_and_customer(message_log):
    await message_log.register("conv-a", "acme", "cust-1")
    await append(message_log, "conv-a", 1)
    from app.replies.models import ChatMessage
    from sqlalchemy import select

    async with message_log.db.session() as session:
        row = await session.scalar(select(ChatMessage))
    assert (row.tenant_id, row.customer_id) == ("acme", "cust-1")


async def test_migration_adds_and_backfills_columns_on_an_old_table():
    """A database created before tenant_id and customer_id existed is upgraded in place."""
    from app.replies.models import migrate
    from sqlalchemy import text

    db = Database(URL)
    async with db.engine.begin() as conn:
        await conn.run_sync(ReplyBase.metadata.drop_all)
        await conn.execute(text(
            "CREATE TABLE chat_conversations (conversation_id VARCHAR(64) PRIMARY KEY, tenant_id VARCHAR(64), "
            "customer_id VARCHAR(128), last_sequence BIGINT, created_at TIMESTAMPTZ)"
        ))  # fmt: skip
        await conn.execute(text(
            "CREATE TABLE chat_messages (id BIGSERIAL PRIMARY KEY, message_id VARCHAR(64), "
            "conversation_id VARCHAR(64), "
            "sequence_number BIGINT, sender_type VARCHAR(32), sender_id VARCHAR(128), sender_name VARCHAR(128), "
            "content TEXT, correlation_id VARCHAR(64), timestamp TIMESTAMPTZ, created_at TIMESTAMPTZ)"
        ))  # fmt: skip
        await conn.execute(text("INSERT INTO chat_conversations VALUES ('c1', 'acme', 'cust-1', 1, now())"))
        await conn.execute(text(
            "INSERT INTO chat_messages (message_id, conversation_id, sequence_number, sender_type, sender_id, "
            "sender_name, content, correlation_id, timestamp, created_at) "
            "VALUES ('m1', 'c1', 1, 'virtual_agent', 'a', '', 'old row', '', now(), now())"
        ))  # fmt: skip
    await migrate(db)
    await migrate(db)  # idempotent
    async with db.engine.connect() as conn:
        row = (await conn.execute(text("SELECT tenant_id, customer_id FROM chat_messages"))).one()
    assert tuple(row) == ("acme", "cust-1")
    async with db.engine.begin() as conn:
        await conn.run_sync(ReplyBase.metadata.drop_all)
    await db.dispose()
