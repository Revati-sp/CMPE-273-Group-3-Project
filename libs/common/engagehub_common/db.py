"""SQLAlchemy async engine helpers shared by every service with a database."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import StaticPool

_SCHEMA_LOCK_ID = 7_219_004


class Database:
    def __init__(self, url: str) -> None:
        kwargs: dict = {"pool_pre_ping": True}
        if url.startswith("sqlite"):
            kwargs.update(poolclass=StaticPool, connect_args={"check_same_thread": False})
        else:
            kwargs.update(pool_size=10, max_overflow=10)
        self.engine: AsyncEngine = create_async_engine(url, **kwargs)
        self._sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        # In-memory SQLite shares one connection, so test transactions must not interleave.
        self._serialised = url.startswith("sqlite")
        self._locks: dict[asyncio.AbstractEventLoop, asyncio.Lock] = {}

    @property
    def dialect(self) -> str:
        return self.engine.dialect.name

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Transaction scope: commits on success, rolls back on any exception."""
        if not self._serialised:
            async with self._sessions() as session, session.begin():
                yield session
            return
        lock = self._locks.setdefault(asyncio.get_running_loop(), asyncio.Lock())
        async with lock, self._sessions() as session, session.begin():
            yield session

    async def create_schema(self, base: type[DeclarativeBase]) -> None:
        async with self.engine.begin() as conn:
            if self.dialect == "postgresql":
                # Replicas may boot together; serialise DDL so create_all cannot race.
                await conn.execute(text(f"SELECT pg_advisory_xact_lock({_SCHEMA_LOCK_ID})"))
            await conn.run_sync(base.metadata.create_all)

    async def ping(self) -> None:
        async with self.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

    async def dispose(self) -> None:
        await self.engine.dispose()

    async def lock_tenant(self, session: AsyncSession, scope: str, tenant_id: str) -> None:
        """Serialise a tenant-wide critical section across replicas (no-op on SQLite)."""
        if self.dialect == "postgresql":
            await session.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": f"{scope}:{tenant_id}"})

    async def insert_ignore(self, session: AsyncSession, model, values: dict) -> bool:
        """INSERT ... ON CONFLICT DO NOTHING. Returns True when a row was inserted."""
        insert = pg_insert if self.dialect == "postgresql" else sqlite_insert
        result = await session.execute(insert(model).values(**values).on_conflict_do_nothing())
        return result.rowcount == 1
